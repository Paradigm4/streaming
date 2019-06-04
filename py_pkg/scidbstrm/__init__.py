import dill
import io
import struct
import sys

# Workaround for NumPy bug #10338
# https://github.com/numpy/numpy/issues/10338
try:
    import numpy
    import pandas
except KeyError:
    import os
    os.environ.setdefault('PATH', '')
    import numpy
    import pandas


__version__ = '18.1.1'


python_map = ("'" +
              'python{major} -uc '.format(major=sys.version_info.major) +
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
        df = pandas.read_feather(io.BytesIO(stdin.read(sz)))
        return df

    else:                       # Last Chunk
        return None


def write(df=None):
    """Write a data chunk to SciDB.

    """
    if df is None:
        stdout.write(struct.pack('<Q', 0))
        return

    buf = io.BytesIO()
    df.to_feather(buf)
    byt = buf.getvalue()
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
