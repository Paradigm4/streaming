#!/bin/bash

# These tests assumes a 2 instance configuration.  A different number
# of instances yield different numbers of computations from the
# stream() operator, which executes on all instances.

MY_DIR=`dirname $0`
pushd $MY_DIR > /dev/null
MY_DIR=`pwd`
EX_DIR=`pwd`/../examples

iquery -aq "remove(foo)" > /dev/null 2>&1
rm -rf $MY_DIR/test.out

iquery -anq "store(build(<val:double>[i=1:800000:0:100000], i), foo)" > /dev/null 2>&1
iquery -aq "op_count(stream(foo, '$EX_DIR/stream_test_client'))" >> $MY_DIR/test.out 2>&1
iquery -aq "stream(foo, '$EX_DIR/stream_test_client')" | head -n 5 >> $MY_DIR/test.out 2>&1
iquery -otsv -aq "stream(_sg(foo, 2,0), '$EX_DIR/stream_test_client SUMMARIZE')" | head -n 1 >> $MY_DIR/test.out 2>&1

#Test READ_DELAY: start a client with a delay
iquery -aq "op_count(stream(foo, '$EX_DIR/stream_test_client READ_DELAY'))" > /dev/null 2>&1 &
sleep 1
#Find the query and cancel it
iquery -aq "op_count(filter(list('queries'), inst=0))" >> $MY_DIR/test.out 2>&1
QID=`iquery -otsv -aq "filter(list('queries'), inst=0 )" | grep stream | awk '{print $1}'`
iquery -aq "cancel('$QID')" >> $MY_DIR/test.out 2>&1
sleep 5
#After 5 seconds, better be cleared up
iquery -aq "op_count(filter(list('queries'), inst=0))" >> $MY_DIR/test.out 2>&1

#Test WRITE_DELAY: start a client with a delay
iquery -aq "op_count(stream(foo, '$EX_DIR/stream_test_client WRITE_DELAY'))" > /dev/null 2>&1 &
sleep 1
#Find the query and cancel it
iquery -aq "op_count(filter(list('queries'), inst=0))" >> $MY_DIR/test.out 2>&1
QID=`iquery -otsv -aq "filter(list('queries'), inst=0 )" | grep stream | awk '{print $1}'`
iquery -aq "cancel('$QID')" >> $MY_DIR/test.out 2>&1
sleep 5
#After 5 seconds, better be cleared up
iquery -aq "op_count(filter(list('queries'), inst=0))" >> $MY_DIR/test.out 2>&1

iquery -ocsv -aq "stream(build(<val:string> [i=1:10:0:5], i), 'Rscript $EX_DIR/tsv_R_client.R')" >> $MY_DIR/test.out 2>&1
iquery -aq "op_count(stream(foo, 'Rscript $EX_DIR/tsv_R_client.R'))" >> $MY_DIR/test.out 2>&1

iquery -aq "remove(foo)" > /dev/null 2>&1

iquery -ocsv -aq "stream(build(<val:double> [i=1:5:0:5], i), 'Rscript $EX_DIR/R_client.R', format:'df', types:('double','int32'))" >> $MY_DIR/test.out 2>&1

iquery -aq "
 stream(
  _sg(
   stream(
    build(<val:double> [i=1:50:0:10], i),
    'Rscript $EX_DIR/R_sum.R',
    format:'df',
    types:'double',
    names:'sum'
   ),
   2, 0
  ),
  'Rscript $EX_DIR/R_sum.R',
  format:'df',
  types:'double',
  names:'sum'
 )" >> $MY_DIR/test.out 2>&1

iquery -ocsv -aq "stream(build(<val:string> [i=1:10,10,0], iif(i=1,'', string(i))), 'Rscript $EX_DIR/R_strings.R', format:'df', types:'string', names:'s')" >> $MY_DIR/test.out 2>&1

iquery -ocsv -aq "stream(
 apply(
  build(<a:int32>[i=1:3:0:3], i),
  dub, double(iif(a=1, null, iif(a=2, 0,  1))),
  i32, int32( iif(a=1, null, iif(a=2, 0,  1))),
  str,        iif(a=1, null, iif(a=2, '', 'abc'))
 ),
 'Rscript $EX_DIR/R_identity.R', format:'df', types:('int32','double','int32','string'), names:('a','b','c','d'))" >> $MY_DIR/test.out 2>&1

#Conversion from client->scidb and then scidb->iquery adds extra backslashes; bear with us!
iquery -otsv -aq "stream(apply(build(<a:string> [i=0:0:0:1], '\n \r \t \ '), b, string(null)), '$EX_DIR/stream_test_client')" >> $MY_DIR/test.out 2>&1

iquery -ocsv -aq "stream(build(<a:double>[i=0:9:0:10],i), 'python $EX_DIR/python_example.py')" >> $MY_DIR/test.out 2>&1

diff $MY_DIR/test.expected $MY_DIR/test.out
