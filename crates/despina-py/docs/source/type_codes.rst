Type Codes, Precision, and File Size
====================================

Type codes define how each table is encoded on disk and are part of the table
schema, not merely an export option. The type code determines the numeric
precision and storage size of each table.

When You Choose Type Codes
--------------------------

One type code is assigned per table when creating a matrix with
:func:`despina.create`. When reading an existing file, type codes are stored in
the file header and can be inspected via :attr:`~despina.matrix.Matrix.tables`.

.. code-block:: python

   import despina

   matrix = despina.create(
       120,
       [
           ("DIST_AM", "D"),
           ("TIME_AM", "S"),
           ("COST_AM", "2"),
       ],
   )

Supported Type Codes
--------------------

The API accepts the following tokens:

1. ``"0"`` to ``"9"`` (or integers ``0`` to ``9``) for fixed decimal places.
2. ``"S"`` for float32 storage.
3. ``"D"`` for float64 storage.

What Each Option Means
----------------------

The fixed decimal codes (``"0"`` through ``"9"``) store values with an explicit
decimal scale, where the digit specifies the number of decimal places preserved
on disk. ``"S"`` stores values at float32 precision and ``"D"`` at float64
precision.

Fixed Decimal Range Examples
----------------------------

For fixed decimal codes, more decimal places means a lower maximum representable
value.

- ``"0"``: about ``4,294,967,295``
- ``"2"``: about ``42,949,672.95``
- ``"4"``: about ``429,496.7295``
- ``"9"``: about ``4.294967295``

How Values Look in Python
-------------------------

Table arrays are always returned as ``float64`` NumPy arrays regardless of the
on-disk type code. On write, values are encoded according to each table's type
code, so precision and range constraints apply at the serialisation boundary.

Inspect Type Codes
------------------

:attr:`~despina.matrix.Matrix.tables` returns a tuple of
:class:`~despina.matrix.TableMeta` objects, each carrying the table's name,
index, and type code.

.. code-block:: python

   import despina

   matrix = despina.read("skims.mat")

   for meta in matrix.tables:
       print(f"{meta.index:>2}  {meta.name:<16}  {meta.type_code}")
