## Python 2.7
## pip install pandas feather-format
## python -u python_feather_example.py
## iquery -aq "stream(apply(build(<a:int64>[i=1:10:0:5], int64(random() % 5)), b, random() % 10), 'python -u /arrow/stream/examples/python_feather_example.py', 'format=feather')"

import sys
import pandas
import io
import struct

end_of_interaction = 0

sys.stderr.write('-- - start - --\n')

while (end_of_interaction != 1):

  ## Read Chunk
  sys.stderr.write('read 8 bytes...')
  sz = struct.unpack('<q', sys.stdin.read(8))[0]
  sys.stderr.write('OK\n')

  if sz:
    sys.stderr.write('read %s bytes...' % sz)
    df = pandas.read_feather(io.BytesIO(sys.stdin.read(sz)))
    sys.stderr.write('OK\n')

    sys.stderr.write('len(df): %d\n' % len(df))
    sys.stderr.write('%s\n' % df)

  else:                         # Last Chunk
    end_of_interaction = 1
    sys.stderr.write("Got size 0. Exiting. Might get killed.\n")

  ## Write Chunk
  df = pandas.DataFrame({'x':[1,2,3], 'y':[10, 20, 30]})
  buf = io.BytesIO()
  df.to_feather(buf)
  byt = buf.getvalue()
  sz = len(byt)

  sys.stderr.write('write 8 + {} bytes...'.format(sz))
  sys.stdout.write(struct.pack('<q', sz))
  sys.stdout.write(byt)
  sys.stderr.write('OK\n')

sys.stderr.write('-- - stop - --\n')
