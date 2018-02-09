#!/bin/sh

set -o errexit


# Install Paradigm4 prerequisites
for pkg in libboost-filesystem1.54.0_1.54.0-2.1_amd64.deb \
           libboost-system1.54.0_1.54.0-2.1_amd64.deb
do
    wget https://downloads.paradigm4.com/ubuntu14.04/libboost/$pkg
done

dpkg --install                                         \
        libboost-filesystem1.54.0_1.54.0-2.1_amd64.deb \
        libboost-system1.54.0_1.54.0-2.1_amd64.deb


# Install prerequisites
## https://github.com/red-data-tools/packages.red-data-tools.org#ubuntu
## No packages for Debian jessie, use Ubuntu trusty
cat <<APT_LINE | tee /etc/apt/sources.list.d/red-data-tools.list
deb https://packages.red-data-tools.org/ubuntu/ trusty universe
deb-src https://packages.red-data-tools.org/ubuntu/ trusty universe
APT_LINE

apt-get update
apt-get install --assume-yes --no-install-recommends --allow-unauthenticated \
        red-data-tools-keyring
apt-get update
apt-get install --assume-yes --no-install-recommends \
        R-base-core                                  \
        cython3                                      \
        libarrow-dev=$ARROW_VER                      \
        libarrow0=$ARROW_VER                         \
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
