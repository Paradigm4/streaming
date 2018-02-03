#!/bin/sh

set -o errexit

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
        libarrow0=$ARROW_VER                         \
        libarrow-dev=$ARROW_VER                      \
        R-base-core


# Compile and install plugin
make --directory /stream
cp /stream/libstream.so /opt/scidb/$SCIDB_VER/lib/scidb/plugins/
iquery --afl --query "load_library('stream')"


# Install Python requirements and SciDB-Strm
wget --no-verbose https://bootstrap.pypa.io/get-pip.py
python get-pip.py
pip install --upgrade -r /stream/py_pkg/requirements.txt
pip install /stream/py_pkg
