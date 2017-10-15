SciDB-Strm: Python Library for SciDB Streaming
==============================================

Requirements
------------

* SciDB ``16.9``
* Apache Arrow ``0.6.0`` or newer
* Python ``2.7.x``; required Python packages::

  feather-format
  dill
  pandas

Installation
------------

Install Apache Arrow
^^^^^^^^^^^^^^^^^^^^

Follow distribution specific instructions to install the
`red-data-tools
<https://github.com/red-data-tools/packages.red-data-tools.org/blob/master/README.md#package-repository>_`
package repository and the `Apache Arrow C++
<https://github.com/red-data-tools/packages.red-data-tools.org/blob/master/README.md#apache-arrow-c>_`
development library. For Red Hat Enterprise Linux use CentOS
instructions.

Install SciDB-Strm Plugin
^^^^^^^^^^^^^^^^^^^^^^^^^

The easiest way is to first set up `dev_tools
<https://github.com/paradigm4/dev_tools>`_. Then it goes something
like this::

  $ iquery --afl --query "load_library('dev_tools')"
  Query was executed successfully

  $ iquery --afl --query "install_github('paradigm4/stream', 'python')"
  {i} success
  {0} true

  $ iquery --afl --query "load_library('stream')"
  Query was executed successfully

  $ iquery --afl --query "
      stream(
        filter(
          build(<val:double>[i=0:0,1,0],0),
          false),
        'printf \"1\nWhat is up?\n\"')"
  {instance_id,chunk_no} response
  {0,0} 'What is up?'
  {1,0} 'What is up?'
  {2,0} 'What is up?'
  {3,0} 'What is up?'

Install SciDB-Strm Python Library
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Install required packages::

  pip install pandas feather-format dill

Install SciDB-Strm library::

  pip install git+http://github.com/paradigm4/stream.git@python#subdirectory=py_pkg

The Python library needs to be installed on the SciDB server as well
as the SciDB client if Python code is to be send from the client to
the server.


SciDB-Strm Python API
-----------------------

Once installed the *SciDB-Strm* Python library can be imported with
``import scidbstrm``. The library provides a high and low-level access
to the SciDB ``stream`` operator as well as the ability to send Python
code to the SciDB server.

High-level access is provided by the function ``map``:

``map(map_fun, finalize_fun=None)``
  Read SciDB chunks. For each chunk, call ``map_fun`` and stream its
  result back to SciDB. If ``finalize_fun`` is provided, call it after
  all the chunks have been processed.

See `1-map-finalize.py <examples/1-map-finalize.py>`_ for an example
using the ``map`` function. The Python script has to be copied on the
SciDB instance.

Python code can be send to the SciDB server for execution using
the ``pack_func`` and ``read_func`` functions::

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

See `3-read-write.py <examples/3-read-write.py>`_ for an example using the
``read`` and ``write`` functions. The Python script has to be copied
on the SciDB instance.
