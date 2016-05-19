#!/bin/bash

MYDIR=`dirname $0`
pushd $MYDIR > /dev/null
MYDIR=`pwd`

iquery -aq "remove(foo)" > /dev/null 2>&1
rm -rf $MYDIR/test.out

iquery -anq "store(build(<val:double>[i=1:800000,100000,0], i), foo)" > /dev/null 2>&1
iquery -aq "op_count(stream(foo, '$MYDIR/stream_test_client'))" >> $MYDIR/test.out 2>&1
iquery -aq "stream(foo, '$MYDIR/stream_test_client')" | head -n 5 >> $MYDIR/test.out 2>&1

#Test READ_DELAY: start a client with a delay
iquery -aq "op_count(stream(foo, '$MYDIR/stream_test_client READ_DELAY'))" > /dev/null 2>&1 &
sleep 1
#Find the query and cancel it
iquery -aq "op_count(filter(list('queries'), inst=0))" >> $MYDIR/test.out 2>&1
QID=`iquery -otsv -aq "filter(list('queries'), inst=0 )" | grep stream | awk '{print $1}'`
iquery -aq "cancel('$QID')" >> $MYDIR/test.out 2>&1
sleep 5
#After 5 seconds, better be cleared up
iquery -aq "op_count(filter(list('queries'), inst=0))" >> $MYDIR/test.out 2>&1

#Test WRITE_DELAY: start a client with a delay
iquery -aq "op_count(stream(foo, '$MYDIR/stream_test_client WRITE_DELAY'))" > /dev/null 2>&1 &
sleep 1
#Find the query and cancel it
iquery -aq "op_count(filter(list('queries'), inst=0))" >> $MYDIR/test.out 2>&1
QID=`iquery -otsv -aq "filter(list('queries'), inst=0 )" | grep stream | awk '{print $1}'`
iquery -aq "cancel('$QID')" >> $MYDIR/test.out 2>&1
sleep 5
#After 5 seconds, better be cleared up
iquery -aq "op_count(filter(list('queries'), inst=0))" >> $MYDIR/test.out 2>&1

diff test.expected test.out
