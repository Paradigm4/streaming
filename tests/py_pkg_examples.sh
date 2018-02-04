#!/bin/bash

set -o errexit

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


# 1.
iquery --afl --query "
    store(
      apply(
        build(<x:int64 not null>[i=1:10:0:5], i),
        y, double(i) * 10 + .1,
        z, 'foo' + string(i)),
      foo)" \
>  $DIR/py_pkg_examples.out

iquery --afl --query "
    stream(
      foo,
      'python -u /stream/py_pkg/examples/1-map-finalize.py',
      'format=feather',
      'types=int64,double,string',
      'names=x,y,info')" \
>> $DIR/py_pkg_examples.out

iquery --afl --query "remove(foo)"


# 2.
python /stream/py_pkg/examples/2-pack-func.py \
>> $DIR/py_pkg_examples.out


# 3.
iquery --afl --query "
    store(
      apply(
        build(<x:int64 not null>[i=1:10:0:5], i),
        y, double(i) * 10 + .1,
        z, 'foo' + string(i)),
      foo)" \
>> $DIR/py_pkg_examples.out

iquery --afl --query "
    stream(
      foo,
      'python -u /stream/py_pkg/examples/3-read-write.py',
      'format=feather',
      'types=int64,double,string')" \
>> $DIR/py_pkg_examples.out

iquery --afl --query "remove(foo)"


# 4.
python /stream/py_pkg/examples/4-machine-learning.py \
>> $DIR/py_pkg_examples.out


# Diff
diff $DIR/py_pkg_examples.out $DIR/py_pkg_examples.expected
