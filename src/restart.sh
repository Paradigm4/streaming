#!/bin/bash

SCIDB_BIN=`which scidb`
SCIDB_BIN=`dirname $SCIDB_BIN`
CONFIG_NAME="mydb"

echo "Using scidb at " $SCIDB_BIN
echo "Using config " $CONFIG_NAME


iquery -aq "unload_library('stream')"
set -e
set -x
make
scidb.py stopall $CONFIG_NAME
cp libstream.so $SCIDB_BIN/../lib/scidb/plugins
scidb.py startall $CONFIG_NAME
iquery -aq "load_library('stream')"
