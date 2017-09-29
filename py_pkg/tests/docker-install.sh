#!/bin/sh


# Setup Bintray.com/rvernica repository for libarrow-dev
apt-key adv --keyserver hkp://keyserver.ubuntu.com --recv 46BD98A354BA5235
echo "deb https://dl.bintray.com/rvernica/deb trusty universe" \
     > /etc/apt/sources.list.d/bintray.list


# Install dev_tools requirements and libarrow-dev
apt-get update
apt-get install --assume-yes  --no-install-recommends \
        bc                                            \
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
pip install /stream/py_pkg/scidbstrm
