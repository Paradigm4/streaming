# BEGIN_COPYRIGHT
#
# Copyright (C) 2017-2021 Paradigm4 Inc.
# All Rights Reserved.
#
# scidbbridge is a plugin for SciDB, an Open Source Array DBMS
# maintained by Paradigm4. See http://www.paradigm4.com/
#
# scidbbridge is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as
# published by the Free Software Foundation.
#
# scidbbridge is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY
# KIND, INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See the
# AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public
# License along with scidbbridge. If not, see
# <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT

import dill
import struct
import sys
import pyarrow

# Workaround for NumPy bug #10338
# https://github.com/numpy/numpy/issues/10338
try:
    import numpy
except KeyError:
    import os
    os.environ.setdefault('PATH', '')
    import numpy


__version__ = '19.11.3'


python_map = ("'" +
              'python{major}.{minor} -uc '.format(
                  major=sys.version_info.major,
                  minor=sys.version_info.minor) +
              '"import scidbstrm; scidbstrm.map(scidbstrm.read_func())"' +
              "'")


# Python 2 and 3 compatibility fix for reading/writing binary data
# to/from STDIN/STDOUT
if hasattr(sys.stdout, 'buffer'):
    # Python 3
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
else:
    # Python 2
    stdin = sys.stdin
    stdout = sys.stdout


def read():
    """Read a data chunk from SciDB. Returns a Pandas DataFrame or None.

    """
    sz = struct.unpack('<Q', stdin.read(8))[0]

    if sz:
        stream = pyarrow.ipc.open_stream(stdin)
        df = stream.read_pandas()
        return df

    else:                       # Last Chunk
        return None


def write(df=None):
    """Write a data chunk to SciDB.

    """
    if df is None:
        stdout.write(struct.pack('<Q', 0))
        return

    buf = pyarrow.BufferOutputStream()
    table = pyarrow.Table.from_pandas(df)
    table = table.replace_schema_metadata()  # Remove metadata
    writer = pyarrow.RecordBatchStreamWriter(buf, table.schema)
    writer.write_table(table)
    writer.close()
    byt = buf.getvalue().to_pybytes()
    sz = len(byt)

    stdout.write(struct.pack('<Q', sz))
    stdout.write(byt)


def pack_func(func):
    """Serialize function to upload to SciDB. The result can be used as
    `upload_data` in `input` or `load` operators.

    """
    return numpy.array(
        [dill.dumps(func, 0)]  # Serialize streaming function
    )


def read_func():
    """Read and de-serialize function from SciDB.

    """
    func = dill.loads(read().iloc[0, 0])
    write()                     # SciDB expects a message back
    return func


def map(map_fun, finalize_fun=None):
    """Read SciDB chunks. For each chunk, call `map_fun` and stream its
    result back to SciDB. If `finalize_fun` is provided, call it after
    all the chunks have been processed.

    """
    while True:

        # Read DataFrame
        df = read()

        if df is None:
            # End of stream
            break

        # Write DataFrame
        write(map_fun(df))

    # Write final DataFrame (if any)
    if finalize_fun is None:
        write()
    else:
        write(finalize_fun())


def debug(*args):
    """Print debug message to scidb-stderr.log file"""
    sys.stderr.write(' '.join('{}'.format(i) for i in args) + '\n')
    sys.stderr.flush()
