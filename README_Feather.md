# Ubuntu 14.04 / Python 2.7

## Install Arrow Library

    # cat /etc/lsb-release
    DISTRIB_ID=Ubuntu
    DISTRIB_RELEASE=14.04
    DISTRIB_CODENAME=trusty
    DISTRIB_DESCRIPTION="Ubuntu 14.04.4 LTS"

    # apt-key adv --keyserver hkp://keyserver.ubuntu.com --recv 46BD98A354BA5235
    Executing: gpg --ignore-time-conflict --no-options --no-default-keyring --homedir /tmp/tmp.Fg1SJYizPp --no-auto-check-trustdb --trust-model always --keyring /etc/apt/trusted.gpg --primary-keyring /etc/apt/trusted.gpg --keyserver hkp://keyserver.ubuntu.com --recv 46BD98A354BA5235
    gpg: requesting key 54BA5235 from hkp server keyserver.ubuntu.com
    gpg: key 54BA5235: public key "Rares Vernica (Bintray) <rvernica@gmail.com>" imported
    gpg: Total number processed: 1
    gpg:               imported: 1  (RSA: 1)

    # echo "deb https://dl.bintray.com/rvernica/deb trusty universe" > /etc/apt/sources.list.d/bintray.list

    # apt-get update
    Get:1 https://dl.bintray.com trusty InRelease
    Get:2 https://dl.bintray.com trusty/universe amd64 Packages
    ...
    Fetched 949 B in 18s (50 B/s)
    Reading package lists... Done

    # apt-get install libarrow-dev
    Reading package lists... Done
    Building dependency tree
    Reading state information... Done
    The following extra packages will be installed:
      libarrow0 libboost-filesystem1.54.0 libboost-system1.54.0 libjemalloc1
    The following NEW packages will be installed:
      libarrow-dev libarrow0 libboost-filesystem1.54.0 libboost-system1.54.0
      libjemalloc1
    0 upgraded, 5 newly installed, 0 to remove and 52 not upgraded.
    Need to get 1399 kB of archives.
    After this operation, 8057 kB of additional disk space will be used.
    Do you want to continue? [Y/n] y
    Get:1 http://archive.ubuntu.com/ubuntu/ trusty-updates/main libboost-system1.54.0 amd64 1.54.0-4ubuntu3.1 [10.1 kB]
    Get:2 http://archive.ubuntu.com/ubuntu/ trusty-updates/main libboost-filesystem1.54.0 amd64 1.54.0-4ubuntu3.1 [34.2 kB]
    Get:3 http://archive.ubuntu.com/ubuntu/ trusty/universe libjemalloc1 amd64 3.5.1-2 [76.8 kB]
    Get:4 https://dl.bintray.com/rvernica/deb/ trusty/universe libarrow0 amd64 0.6.0-1 [791 kB]
    Get:5 https://dl.bintray.com/rvernica/deb/ trusty/universe libarrow-dev amd64 0.6.0-1 [487 kB]
    Fetched 1399 kB in 8s (165 kB/s)
    Selecting previously unselected package libboost-system1.54.0:amd64.
    (Reading database ... 12026 files and directories currently installed.)
    Preparing to unpack .../libboost-system1.54.0_1.54.0-4ubuntu3.1_amd64.deb ...
    Unpacking libboost-system1.54.0:amd64 (1.54.0-4ubuntu3.1) ...
    Selecting previously unselected package libboost-filesystem1.54.0:amd64.
    Preparing to unpack .../libboost-filesystem1.54.0_1.54.0-4ubuntu3.1_amd64.deb ...
    Unpacking libboost-filesystem1.54.0:amd64 (1.54.0-4ubuntu3.1) ...
    Selecting previously unselected package libjemalloc1.
    Preparing to unpack .../libjemalloc1_3.5.1-2_amd64.deb ...
    Unpacking libjemalloc1 (3.5.1-2) ...
    Selecting previously unselected package libarrow0:amd64.
    Preparing to unpack .../libarrow0_0.6.0-1_amd64.deb ...
    Unpacking libarrow0:amd64 (0.6.0-1) ...
    Selecting previously unselected package libarrow-dev:amd64.
    Preparing to unpack .../libarrow-dev_0.6.0-1_amd64.deb ...
    Unpacking libarrow-dev:amd64 (0.6.0-1) ...
    Setting up libboost-system1.54.0:amd64 (1.54.0-4ubuntu3.1) ...
    Setting up libboost-filesystem1.54.0:amd64 (1.54.0-4ubuntu3.1) ...
    Setting up libjemalloc1 (3.5.1-2) ...
    Setting up libarrow0:amd64 (0.6.0-1) ...
    Setting up libarrow-dev:amd64 (0.6.0-1) ...
    Processing triggers for libc-bin (2.19-0ubuntu6.9) ...
    #

## Setup Python

    pip install pandas feather-format

## Build and Install Plugin

## Run

Adjust path to `python_feather_example.py`:

    iquery -aq "
      parse(
        stream(
          apply(
            build(<a:int64>[i=1:10:0:5], int64(random() % 5)),
            b, random() % 10),
          'python -u examples/python_feather_example.py',
          'format=feather'),
        'num_attributes=1')"
