#!/bin/sh

set -o errexit


# Install prerequisites
## libboost-system1.54 and libboost-filesyste1.54
cat <<APT_LINE | tee /etc/apt/sources.list.d/trusty.list
deb http://us.archive.ubuntu.com/ubuntu/ trusty main
deb http://us.archive.ubuntu.com/ubuntu/ trusty-updates main
APT_LINE
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 40976EAF437D05B5
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 3B4FE6ACC0B21F32

## https://github.com/red-data-tools/packages.red-data-tools.org#ubuntu
## No packages for Debian jessie, use Ubuntu trusty
cat <<APT_LINE | tee /etc/apt/sources.list.d/red-data-tools.list
deb https://packages.red-data-tools.org/ubuntu/ trusty universe
APT_LINE

apt-get update
apt-get install --assume-yes --no-install-recommends --allow-unauthenticated \
        red-data-tools-keyring
apt-get update
apt-get install --assume-yes --no-install-recommends \
        libarrow-dev=$ARROW_VER                      \
        libarrow0=$ARROW_VER
apt-get install                                      \
        --assume-yes                                 \
        --no-install-recommends                      \
        --target-release jessie-updates              \
        R-base-core                                  \
        cython3                                      \
        g++                                          \
        python3                                      \
        python3-dev


# Compile and install plugin
iquery --afl --query "unload_library('stream')"
scidb.py stopall scidb
make --directory /stream
cp /stream/libstream.so /opt/scidb/$SCIDB_VER/lib/scidb/plugins/
scidb.py startall scidb
iquery --afl --query "load_library('stream')"


# Install Python requirements and SciDB-Strm
wget --no-verbose https://bootstrap.pypa.io/get-pip.py

python2 get-pip.py
pip2 install --upgrade -r /stream/py_pkg/requirements.txt
pip2 install /stream/py_pkg

python3 get-pip.py
pip3 install --upgrade -r /stream/py_pkg/requirements.txt
pip3 install /stream/py_pkg
