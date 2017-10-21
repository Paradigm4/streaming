import dill
import io
import numpy
import pandas
import struct
import sys

__version__ = '16.9.0'


python_map = ("'" +
              'python -uc ' +
              '"import scidbstrm; scidbstrm.map(scidbstrm.read_func())"' +
              "'")


def read():
    """Read a data chunk from SciDB. Returns a Pandas DataFrame or None.

    """
    sys.stderr.write('read 8 bytes\n')
    sz = struct.unpack('<Q', sys.stdin.read(8))[0]

    if sz:
        sys.stderr.write('read %s bytes\n' % sz)
        df = pandas.read_feather(io.BytesIO(sys.stdin.read(sz)))

        sys.stderr.write('len(df): %d\n' % len(df))
        return df

    else:                       # Last Chunk
        sys.stderr.write('got size 0\n')
        return None


def write(df=None):
    """Write a data chunk to SciDB.

    """
    if df is None:
        sys.stderr.write('write size 0\n')
        sys.stdout.write(struct.pack('<Q', 0))
        return

    buf = io.BytesIO()
    df.to_feather(buf)
    byt = buf.getvalue()
    sz = len(byt)

    sys.stderr.write('write 8 + {} bytes\n'.format(sz))
    sys.stdout.write(struct.pack('<Q', sz))
    sys.stdout.write(byt)


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
    sys.stderr.write('-- - start - --\n')
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
    sys.stderr.write('-- - stop - --\n')
