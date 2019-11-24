#!/bin/sh

set -o errexit


# Install prerequisites
# ## https://github.com/red-data-tools/packages.red-data-tools.org#ubuntu
# ## No packages for Debian jessie, use Ubuntu trusty
# cat <<APT_LINE | tee /etc/apt/sources.list.d/red-data-tools.list
# deb https://packages.red-data-tools.org/ubuntu/ trusty universe
# APT_LINE

wget -O- https://paradigm4.github.io/extra-scidb-libs/install.sh \
|  sh -s -- --only-prereq

sed --in-place                                                  \
    "\#deb http://deb.debian.org/debian jessie-updates main#d"  \
    /etc/apt/sources.list

# apt-get update
# apt-get install --assume-yes --no-install-recommends --allow-unauthenticated \
#         red-data-tools-keyring
apt-get update
apt-get install --assume-yes --no-install-recommends \
        libarrow-dev=$ARROW_VER-1
apt-get install                                      \
        --assume-yes                                 \
        --no-install-recommends                      \
        R-base-core                                  \
        cython3                                      \
        g++                                          \
        python3                                      \
        python3-dev


# Compile and install plugin
# iquery --afl --query "unload_library('stream')"
# scidbctl.py stop $SCIDB_NAME
make --directory /stream
cp /stream/libstream.so /opt/scidb/$SCIDB_VER/lib/scidb/plugins/
# scidbctl.py start $SCIDB_NAME
iquery --afl --query "load_library('stream')"


# Install Python requirements and SciDB-Strm
wget --no-verbose https://bootstrap.pypa.io/get-pip.py

python2 get-pip.py
pip2 install --upgrade -r /stream/py_pkg/requirements.txt
pip2 install /stream/py_pkg

python3 get-pip.py
pip3 install numpy==1.14.0 # pandas==0.19.0
pip3 install --upgrade -r /stream/py_pkg/requirements.txt
pip3 install /stream/py_pkg
