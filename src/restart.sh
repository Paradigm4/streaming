#!/bin/bash

SCIDB_BIN=`which scidb`
SCIDB_BIN=`dirname $SCIDB_BIN`
CONFIG_NAME=`iquery -otsv -aq "project(filter(list('instances'), No=0), instance_path)" | sed -s "s/.*DB-//g" | sed -s "s/\/0\/0//g"`

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
