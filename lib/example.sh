#!/bin/bash
#
# Example script that reads messages formatted as follows:
# n
# line 1
# line 2
# ...
# line n
#
# The program reads blocks of n lines indefinitely untile
# n is less than 1, and then exits when that occurs.

while true; do
  read n                    # number of lines to read
  [ $n -lt 1 ] && exit 0    # exit nicely if n < 1
  j=0
  echo $n                   # signal that we're going to write n lines back
  while test $j -lt $n; do  # read n lines
    read x
    echo $j $x              # print line with number
    j=$(( $j + 1 ))
  done
done
