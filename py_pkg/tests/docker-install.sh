#!/bin/sh

## https://github.com/red-data-tools/packages.red-data-tools.org#debian-gnulinux
cat <<APT_LINE | tee /etc/apt/sources.list.d/red-data-tools.list
deb https://packages.red-data-tools.org/ubuntu/ trusty universe
deb-src https://packages.red-data-tools.org/ubuntu/ trusty universe
APT_LINE

apt-get update
apt-get install --assume-yes --no-install-recommends --allow-unauthenticated \
        red-data-tools-keyring
apt-get update
apt-get install --assume-yes --no-install-recommends \
        bc                                           \
        libarrow-dev


# Install dev_tools
SCIDB_DEV_TOOLS=332127216cd6d5791320abafd767e09164cd22e2
wget --no-verbose --output-document -                                       \
     https://github.com/Paradigm4/dev_tools/archive/$SCIDB_DEV_TOOLS.tar.gz \
    |  tar --extract --gzip --directory=/usr/local/src                      \
    && make --directory=/usr/local/src/dev_tools-$SCIDB_DEV_TOOLS           \
    && cp /usr/local/src/dev_tools-$SCIDB_DEV_TOOLS/*.so                    \
          $SCIDB_INSTALL_PATH/lib/scidb/plugins


# Install Python requirements and SciDB-Strm
wget --no-verbose https://bootstrap.pypa.io/get-pip.py
python get-pip.py
pip install --upgrade -r /stream/py_pkg/requirements.txt
pip install /stream/py_pkg
