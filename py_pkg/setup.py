#!/usr/bin/env python

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
        'pandas',
        'pyarrow==0.9.0;python_version<"3.5"',
        'pyarrow;python_version>="3.5"',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Database :: Front-Ends',
        'Topic :: Scientific/Engineering',
    ],
)
