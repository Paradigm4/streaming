SciDB-Strm: Python Library for SciDB Streaming
==============================================

Requirements
------------

Operating system:

* CentOS ``6``
* Red Hat Enterprise Linux ``6``
* Ubuntu ``14.04``

Software:

* SciDB ``16.9``
* Apache Arrow ``0.6.0``
* Python ``2.7.x``; required Python packages::

  feather-format
  pandas

Installation
------------

Install Apache Arrow
^^^^^^^^^^^^^^^^^^^^

CentOS6
.......

::

  # cat /etc/redhat-release
  CentOS release 6.9 (Final)

  # yum install epel-release
  ...

  # cat > /etc/yum.repos.d/bintray.repo
  [bintray--kou-apache-arrow-yum]
  name=bintray--kou-apache-arrow-yum
  baseurl=https://dl.bintray.com/kou/apache-arrow-yum/centos/6/x86_64
  gpgcheck=0
  repo_gpgcheck=0
  enabled=1

  # yum info arrow-devel
  Loaded plugins: fastestmirror, ovl
  Loading mirror speeds from cached hostfile
   * base: mirror.sjc02.svwh.net
   * extras: mirror.keystealth.org
   * updates: mirror.nodesdirect.com
  Available Packages
  Name        : arrow-devel
  Arch        : x86_64
  Version     : 0.6.0
  Release     : 1.el6
  Size        : 585 k
  Repo        : bintray--kou-apache-arrow-yum
  Summary     : Libraries and header files for Apache Arrow C++
  URL         : https://arrow.apache.org/
  License     : Apache-2.0
  Description : Libraries and header files for Apache Arrow C++.

  # yum install arrow-devel
  ...
  ================================================================================
   Package           Arch    Version         Repository                      Size
  ================================================================================
  Installing:
   arrow-devel       x86_64  0.6.0-1.el6     bintray--kou-apache-arrow-yum  585 k
  Installing for dependencies:
   arrow-libs        x86_64  0.6.0-1.el6     bintray--kou-apache-arrow-yum  911 k
   boost-filesystem  x86_64  1.41.0-28.el6   base                            47 k
   boost-system      x86_64  1.41.0-28.el6   base                            26 k
   jemalloc          x86_64  3.6.0-1.el6     epel                           100 k

  Transaction Summary
  ================================================================================
  ...
  (1/5): arrow-devel-0.6.0-1.el6.x86_64.rpm                | 585 kB     00:01
  (2/5): arrow-libs-0.6.0-1.el6.x86_64.rpm                 | 911 kB     00:04
  (5/5): jemalloc-3.6.0-1.el6.x86_64.rpm                   | 100 kB     00:00
  --------------------------------------------------------------------------------
  Total                                           142 kB/s | 1.6 MB     00:11
  warning: rpmts_HdrFromFdno: Header V3 RSA/SHA256 Signature, key ID 0608b895: NOKEY
  Retrieving key from file:///etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-6
  Importing GPG key 0x0608B895:
   Userid : EPEL (6) <epel@fedoraproject.org>
   Package: epel-release-6-8.noarch (@extras)
   From   : /etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-6
  Is this ok [y/N]: y
  ...
  Installed:
    arrow-devel.x86_64 0:0.6.0-1.el6

  Dependency Installed:
    arrow-libs.x86_64 0:0.6.0-1.el6      boost-filesystem.x86_64 0:1.41.0-28.el6
    boost-system.x86_64 0:1.41.0-28.el6  jemalloc.x86_64 0:3.6.0-1.el6

  Complete!

Ubuntu 14.04
............

::

  # cat /etc/lsb-release
  DISTRIB_ID=Ubuntu
  DISTRIB_RELEASE=14.04
  DISTRIB_CODENAME=trusty
  DISTRIB_DESCRIPTION="Ubuntu 14.04.4 LTS"

  # apt-get update
  ...
  # apt-get install apt-transport-https
  ...

  # apt-key adv --keyserver hkp://keyserver.ubuntu.com --recv 46BD98A354BA5235
  Executing: gpg --ignore-time-conflict --no-options --no-default-keyring --homedir /tmp/tmp.Fg1SJYizPp --no-auto-check-trustdb --trust-model always --keyring /etc/apt/trusted.gpg --primary-keyring /etc/apt/trusted.gpg --keyserver hkp://keyserver.ubuntu.com --recv 46BD98A354BA5235
  gpg: requesting key 54BA5235 from hkp server keyserver.ubuntu.com
  gpg: key 54BA5235: public key "Rares Vernica (Bintray) <rvernica@gmail.com>" imported
  gpg: Total number processed: 1
  gpg:               imported: 1  (RSA: 1)

  # echo "deb https://dl.bintray.com/rvernica/deb trusty universe" > /etc/apt/sources.list.d/bintray.list

  # apt-get update
  ...
  Get:3 https://dl.bintray.com trusty InRelease
  Get:5 https://dl.bintray.com trusty Release.gpg
  Get:6 https://dl.bintray.com trusty Release
  ...

  # apt-cache show libarrow-dev
  Package: libarrow-dev
  Source: apache-arrow
  Version: 0.6.0-1
  Architecture: amd64
  Maintainer: Kouhei Sutou <kou@clear-code.com>
  Installed-Size: 4730
  Depends: libarrow0 (= 0.6.0-1)
  Section: libdevel
  Priority: optional
  Multi-Arch: same
  Homepage: https://arrow.apache.org/
  Description: Apache Arrow is a data processing library for analysis
   .
   This package provides header files.
  Description-md5: e4855d5dbadacb872bf8c4ca67f624e3
  Filename: libarrow-dev_0.6.0-1_amd64.deb
  SHA1: 8810a058c1dcc6c8c274c808e3f16ba28cdd0318
  SHA256: b246fff141219d7ef01d4c0689f9137f1efb435293fc37f0628b1a909e7954

  # apt-get install libarrow-dev
  Reading package lists... Done
  Building dependency tree
  Reading state information... Done
  The following extra packages will be installed:
    libarrow0 libboost-filesystem1.54.0 libboost-system1.54.0 libjemalloc1
  The following NEW packages will be installed:
    libarrow-dev libarrow0 libboost-filesystem1.54.0 libboost-system1.54.0
    libjemalloc1
  ...
  Do you want to continue? [Y/n] y
  ...
  Get:3 http://archive.ubuntu.com/ubuntu/ trusty/universe libjemalloc1 amd64 3.5.1-2 [76.8 kB]
  Get:4 https://dl.bintray.com/rvernica/deb/ trusty/universe libarrow0 amd64 0.6.0-1 [791 kB]
  Get:5 https://dl.bintray.com/rvernica/deb/ trusty/universe libarrow-dev amd64 0.6.0-1 [487 kB]
  ...
  Setting up libjemalloc1 (3.5.1-2) ...
  Setting up libarrow0:amd64 (0.6.0-1) ...
  Setting up libarrow-dev:amd64 (0.6.0-1) ...

Install SciDB Stream Plugin
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The easiest way is to first set up `dev_tools
<https://github.com/paradigm4/dev_tools>`_. Then it goes something
like this::

  $ iquery --afl --query "load_library('dev_tools')"
  Query was executed successfully

  $ iquery --afl --query "install_github('paradigm4/stream')"
  {i} success
  {0} true

  $ iquery --afl --query "load_library('stream', 'python')"
  Query was executed successfully

  $ iquery --afl --query "
      stream(
        filter(
          build(<val:double>[i=0:0,1,0],0),
          false),
        'printf \"1\nWhat is up?\n\"')"
  {instance_id,chunk_no} response
  {0,0} 'What is up?'
  {1,0} 'What is up?'
  {2,0} 'What is up?'
  {3,0} 'What is up?'

Install SciDB-Stream Python Package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Install required packages::

  pip install pandas feather-format

Install SciDB-Stream package::

  pip install git+http://github.com/paradigm4/stream.git@python#subdirectory=py_pkg

SciDB-Strm Python API
-----------------------

Once installed the SciDB-Stream Python library can be imported with
``import scidbstrm``. The library provides a high and low level access
to the SciDB ``stream`` operator.

High-level access is provided by the function ``map``:

``map(map_fun, finalize_fun=None)``
  Read SciDB chunks. For each chunk, call ``map_fun`` and stream its
  result back to SciDB. If ``finalize_fun`` is provided, call it after
  all the chunks have been processed.

See `example_high.py<example_high.py>`_ for an example using the
``map`` function.

Low-level access is provided by the ``read`` and ``write`` functions:

``read()``
  Read a data chunk from SciDB. Returns a Pandas DataFrame or None.

``write(df=None)``
  Write a data chunk to SciDB.

See `example_low.py<example_low.py>`_ for an example using the
``read`` and ``write`` functions.
