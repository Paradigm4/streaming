"""Usage:

# Setup
> iquery --afl --query "
    store(
      apply(
        build(<x:int64 not null>[i=1:10:0:5], i),
        y, double(i) * 10 + .1,
        z, 'foo' + string(i)),
      foo)"
{i} x,y,z
{1} 1,10.1,'foo1'
{2} 2,20.1,'foo2'
{3} 3,30.1,'foo3'
{4} 4,40.1,'foo4'
{5} 5,50.1,'foo5'
{6} 6,60.1,'foo6'
{7} 7,70.1,'foo7'
{8} 8,80.1,'foo8'
{9} 9,90.1,'foo9'
{10} 10,100.1,'foo10'


# Adjust the path to `3-read-write.py` script to match your setup
> iquery --afl --query "
    stream(
      foo,
      'python -u /stream/py_pkg/examples/3-read-write.py',
      format:'feather',
      types:('int64','double','string'))"
{instance_id,chunk_no,value_no} a0,a1,a2
{0,0,0} 1,10.1,'foo1'
{0,0,1} 2,20.1,'foo2'
{0,0,2} 3,30.1,'foo3'
{0,0,3} 4,40.1,'foo4'
{0,0,4} 5,50.1,'foo5'
{1,0,0} 6,60.1,'foo6'
{1,0,1} 7,70.1,'foo7'
{1,0,2} 8,80.1,'foo8'
{1,0,3} 9,90.1,'foo9'
{1,0,4} 10,100.1,'foo10'


# Cleanup
> iquery --afl --query "remove(foo)"

"""

import scidbstrm
import sys


sys.stderr.write('-- - start - --\n')
while True:

    # Read DataFrame
    df = scidbstrm.read()

    if df is None:
        # End of stream
        break

    # Write DataFrame
    scidbstrm.write(df)

# Write final DataFrame (if any)
scidbstrm.write()
sys.stderr.write('-- - stop - --\n')
