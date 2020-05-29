#!/bin/bash

set -o errexit

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DIR_EXAMPLES=$DIR/../py_pkg/examples

if [ "$1" != "" ]
then
    PRE="$1"
    DIR_EXAMPLES="/stream/py_pkg/examples"
fi

if [ "$TRAVIS_PYTHON_VERSION" != "" ]
then
    PYTHON=python${TRAVIS_PYTHON_VERSION%.*}
else
    PYTHON=python
fi


# 0.
$PRE iquery --afl --query "
  stream(
    build(<x:int64 not null>[i=1:10:0:5], i),
    'python -uc \"

import scidbstrm, pandas

scidbstrm.map(lambda df: pandas.DataFrame({\'count\': [len(df)]}))
\"',
    format:'feather',
    types:'int64',
    names:'count')" \
>  $DIR/py_pkg_examples.out


# 1.
$PRE iquery --afl --query "
    store(
      apply(
        build(<x:int64 not null>[i=1:10:0:5], i),
        y, double(i) * 10 + .1,
        z, 'foo' + string(i)),
      foo);
    scan(foo)" \
>> $DIR/py_pkg_examples.out

$PRE iquery --afl --query "
    stream(
      foo,
      '$PYTHON -u $DIR_EXAMPLES/1-map-finalize.py',
      format:'feather',
      types:('int64','double','string'),
      names:('x','y','info'))" \
>> $DIR/py_pkg_examples.out

$PRE iquery --afl --query "remove(foo)"


# 2.
$PYTHON $DIR/../py_pkg/examples/2-pack-func.py \
>> $DIR/py_pkg_examples.out


# 3.
$PRE iquery --afl --query "
    store(
      apply(
        build(<x:int64 not null>[i=1:10:0:5], i),
        y, double(i) * 10 + .1,
        z, 'foo' + string(i)),
      foo);
    scan(foo)" \
>> $DIR/py_pkg_examples.out

$PRE iquery --afl --query "
    stream(
      foo,
      '$PYTHON -u $DIR_EXAMPLES/3-read-write.py',
      format:'feather',
      types:('int64','double','string'))" \
>> $DIR/py_pkg_examples.out

$PRE iquery --afl --query "remove(foo)"


# 4.
if [ "${TRAVIS_PYTHON_VERSION%.*}" = "2" ]
then
    $PYTHON $DIR/../py_pkg/examples/4-machine-learning.py2.7.py \
            "$DIR_EXAMPLES"                                     \
            >> $DIR/py_pkg_examples.out
else
    $PYTHON $DIR/../py_pkg/examples/4-machine-learning.py       \
            "$DIR_EXAMPLES"                                     \
            >> $DIR/py_pkg_examples.out
fi


# Diff
diff --ignore-trailing-space \
     $DIR/py_pkg_examples.out $DIR/py_pkg_examples.expected
