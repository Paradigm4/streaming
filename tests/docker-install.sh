#!/bin/sh

set -o errexit


wget -O- https://paradigm4.github.io/extra-scidb-libs/install.sh \
|  sh -s -- --only-prereq

id=`lsb_release --id --short`
codename=`lsb_release --codename --short`
if [ "$codename" = "stretch" ]
then
    cat > /etc/apt/sources.list.d/backports.list <<EOF
deb http://deb.debian.org/debian $codename-backports main
EOF
fi
wget https://apache.bintray.com/arrow/$(
    echo $id | tr 'A-Z' 'a-z'
     )/apache-arrow-archive-keyring-latest-$codename.deb
apt install --assume-yes ./apache-arrow-archive-keyring-latest-$codename.deb

apt-get update
apt-get install                              \
        --assume-yes --no-install-recommends \
        libarrow-dev=$ARROW_VER-1            \
        libpqxx-dev                          \
        r-base-core

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
pip2 install --upgrade -r https://github.com/Paradigm4/SciDB-Py/raw/master/requirements.txt
pip2 install --upgrade -r /stream/py_pkg/requirements.txt
pip2 install /stream/py_pkg

wget --no-verbose                               \
     --output-document=get-pip-py3.4.py         \
     https://bootstrap.pypa.io/3.4/get-pip.py
python3 get-pip-py3.4.py
# pip3 install numpy==1.14.0 # pandas==0.19.0
pip3 install --upgrade -r https://github.com/Paradigm4/SciDB-Py/raw/master/requirements.txt
pip3 install --upgrade -r /stream/py_pkg/requirements.txt
pip3 install /stream/py_pkg
