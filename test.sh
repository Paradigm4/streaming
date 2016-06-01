#!/bin/bash

MYDIR=`dirname $0`
pushd $MYDIR > /dev/null
MYDIR=`pwd`

iquery -aq "remove(foo)" > /dev/null 2>&1
rm -rf $MYDIR/test.out

iquery -anq "store(build(<val:double>[i=1:800000,100000,0], i), foo)" > /dev/null 2>&1
iquery -aq "op_count(stream(foo, '$MYDIR/examples/stream_test_client'))" >> $MYDIR/test.out 2>&1
iquery -aq "stream(foo, '$MYDIR/examples/stream_test_client')" | head -n 5 >> $MYDIR/test.out 2>&1
iquery -otsv -aq "stream(_sg(foo, 2,0), '$MYDIR/examples/stream_test_client SUMMARIZE')" | head -n 1 >> $MYDIR/test.out 2>&1

#Test READ_DELAY: start a client with a delay
iquery -aq "op_count(stream(foo, '$MYDIR/examples/stream_test_client READ_DELAY'))" > /dev/null 2>&1 &
sleep 1
#Find the query and cancel it
iquery -aq "op_count(filter(list('queries'), inst=0))" >> $MYDIR/test.out 2>&1
QID=`iquery -otsv -aq "filter(list('queries'), inst=0 )" | grep stream | awk '{print $1}'`
iquery -aq "cancel('$QID')" >> $MYDIR/test.out 2>&1
sleep 5
#After 5 seconds, better be cleared up
iquery -aq "op_count(filter(list('queries'), inst=0))" >> $MYDIR/test.out 2>&1

#Test WRITE_DELAY: start a client with a delay
iquery -aq "op_count(stream(foo, '$MYDIR/examples/stream_test_client WRITE_DELAY'))" > /dev/null 2>&1 &
sleep 1
#Find the query and cancel it
iquery -aq "op_count(filter(list('queries'), inst=0))" >> $MYDIR/test.out 2>&1
QID=`iquery -otsv -aq "filter(list('queries'), inst=0 )" | grep stream | awk '{print $1}'`
iquery -aq "cancel('$QID')" >> $MYDIR/test.out 2>&1
sleep 5
#After 5 seconds, better be cleared up
iquery -aq "op_count(filter(list('queries'), inst=0))" >> $MYDIR/test.out 2>&1

iquery -ocsv -aq "stream(build(<val:string> [i=1:10,5,0], i), 'Rscript $MYDIR/examples/tsv_R_client.R')" >> $MYDIR/test.out 2>&1
iquery -aq "op_count(stream(foo, 'Rscript $MYDIR/examples/tsv_R_client.R'))" >> $MYDIR/test.out 2>&1

iquery -aq "stream(build(<val:double> [i=1:5,5,0], i), 'Rscript $MYDIR/examples/R_client.R', 'format=df', 'types=double,int32')" >> $MYDIR/test.out 2>&1

iquery -aq "
 stream(
  _sg(
   stream(
    build(<val:double> [i=1:50,10,0], i), 
    'Rscript $MYDIR/examples/R_sum.R', 
    'format=df', 
    'types=double', 
    'names=sum'
   ), 
   2, 0
  ), 
  'Rscript $MYDIR/examples/R_sum.R', 
  'format=df', 
  'types=double', 
  'names=sum'
 )" >> $MYDIR/test.out 2>&1

iquery -aq "stream(build(<val:string> [i=1:10,10,0], iif(i=1,'', string(i))), 'Rscript $MYDIR/examples/R_strings.R', 'format=df', 'types=string', 'names=s')" >> $MYDIR/test.out 2>&1

iquery -aq "stream(
 apply(
  build(<a:int32>[i=1:3,3,0], i), 
  dub, double(iif(a=1, null, iif(a=2, 0,  1))),  
  i32, int32( iif(a=1, null, iif(a=2, 0,  1))), 
  str,        iif(a=1, null, iif(a=2, '', 'abc'))
 ), 
 'Rscript $MYDIR/examples/R_identity.R', 'format=df', 'types=int32,double,int32,string', 'names=a,b,c,d'
)" >> $MYDIR/test.out 2>&1

diff test.expected test.out

