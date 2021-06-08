#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright (C) 2017-2021 Paradigm4 Inc.
# All Rights Reserved.
#
# scidbbridge is a plugin for SciDB, an Open Source Array DBMS
# maintained by Paradigm4. See http://www.paradigm4.com/
#
# scidbbridge is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as
# published by the Free Software Foundation.
#
# scidbbridge is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY
# KIND, INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See the
# AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public
# License along with scidbbridge. If not, see
# <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
import scidbstrm


NAME = 'scidb-strm'
DESCRIPTION = 'Python library for SciDB streaming'
LONG_DESCRIPTION = open('README.rst').read()
AUTHOR = 'Rares Vernica'
AUTHOR_EMAIL = 'rvernica@gmail.com'
DOWNLOAD_URL = 'http://github.com/Paradigm4/Stream'
LICENSE = 'AGPL-3.0'
VERSION = scidbstrm.__version__

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    download_url=DOWNLOAD_URL,
    license=LICENSE,
    packages=['scidbstrm'],
    install_requires=[
        'dill',
        'feather-format',
        'pandas>=0.20.0',
        'pyarrow==0.16.0',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Database :: Front-Ends',
        'Topic :: Scientific/Engineering',
    ],
)
