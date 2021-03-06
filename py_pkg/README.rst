SciDB-Strm: Python Library for SciDB Streaming
==============================================

.. image:: https://img.shields.io/badge/SciDB-19.11-blue.svg
    :target: https://forum.paradigm4.com/t/scidb-release-19-11/2411

.. image:: https://img.shields.io/badge/arrow-0.16.0-blue.svg
    :target: https://arrow.apache.org/release/0.16.0.html

.. image:: https://travis-ci.org/Paradigm4/stream.svg
    :target: https://travis-ci.org/Paradigm4/stream

Requirements
------------

SciDB ``19.11`` or newer.

Apache Arrow ``3.0.0``.

Python ``3.5.x``, ``3.6.x``, ``3.7.x``, ``3.8.x``, or ``3.9.x``.

Required Python packages::

  dill
  pandas
  pyarrow

Installation
------------

Install latest release::

  pip install scidb-strm

Install development version from GitHub::

  pip install git+http://github.com/paradigm4/stream.git#subdirectory=py_pkg

The Python library needs to be installed on the SciDB server. The
library needs to be installed on the client as well, if Python code is
to be send from the client to the server.


SciDB-Strm Python API and Examples
----------------------------------

Once installed the *SciDB-Strm* Python library can be imported with
``import scidbstrm``. The library provides a high and low-level access
to the SciDB ``stream`` operator as well as the ability to send Python
code to the SciDB server.

High-level access is provided by the function ``map``:

``map(map_fun, finalize_fun=None)``
  Read SciDB chunks. For each chunk, call ``map_fun`` and stream its
  result back to SciDB. If ``finalize_fun`` is provided, call it after
  all the chunks have been processed.

See `0-iquery.txt <examples/0-iquery.txt>`_ for a succinct example
using the ``map`` function.

See `1-map-finalize.py <examples/1-map-finalize.py>`_ for an example
using the ``map`` function. The Python script has to be copied onto
the SciDB instance.

Python code can be send to the SciDB server for execution using
the ``pack_func`` and ``read_func`` functions:

``pack_func(func)``
  Serialize Python function for use as ``upload_data`` in ``input`` or
  ``load`` operators.

``read_func()``
  Read and de-serialize function from SciDB.

See `2-pack-func.py <examples/2-pack-func.py>`_ for an example of
using the ``pack_func`` and ``read_func`` functions.

Low-level access is provided by the ``read`` and ``write`` functions:

``read()``
  Read a data chunk from SciDB. Returns a Pandas DataFrame or None.

``write(df=None)``
  Write a data chunk to SciDB.

See `3-read-write.py <examples/3-read-write.py>`_ for an example using
the ``read`` and ``write`` functions. The Python script has to be
copied onto the SciDB instance.

A convenience invocation of the Python interpreter is provided in
``python_map`` variable and it is set to::

  python -uc "import scidbstrm; scidbstrm.map(scidbstrm.read_func())"

Finally, see `4-machine-learning.py <examples/4-machine-learning.py>`_
for a more complex example of going through the steps of using
machine learning (preprocessing, training, and prediction).


Debugging Python Code
---------------------

When debugging Python code executed as part of the ``stream`` operator
*do not* use the ``print`` function. The ``stream`` operator
communicates with the Python process using ``stdout``. The ``print``
function writes output to ``stdout``. So, using the ``print`` function
would interfere with the inter-process communication.

Instead, use the ``debug`` function provided by the library. The
function formats the arguments as strings and printed them all out
separated by space. For example::

  debug("Value of i is", 10)

Alternatively, output can be written directly to ``stderr`` using the
``write`` function. For example::

  import sys

  x = [1, 2, 3]
  sys.stderr.write("{}\n".format(x))

The output is written in the ``scidb-stderr.log`` files of each
instance, for example::

  /opt/scidb/18.1/DB-scidb/0/0/scidb-stderr.log
  /opt/scidb/18.1/DB-scidb/0/1/scidb-stderr.log

If using SciDB ``18.1`` installed in the default location and
configured with one server and two instances.


ImportError: No module named
----------------------------

When trying to de-serialize a Python function uploaded to SciDB using
``pack_func``, one might encounter::

  ImportError: No module named ...

This error is because ``dill``, the Python serialization library,
links the function to the module in which it is defined. This can be
resolved in two ways:

1. Make the named module available on all the SciDB instances
2. If the module is small, the recursive ``dill`` mode can be
   used. Replace::

     foo_pack = scidbstrm.pack_func(foo)

   with::

     foo_pack = numpy.array([dill.dumps(foo, 0, recurse=True)])
