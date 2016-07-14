# An R interface to the SciDB Streaming API

## Installation

Run the following steps:
```
R CMD build r_pkg
sudo R CMD INSTALL scidbstrm_VERSION_NUMBER.tar.gz # Find the version number from tar.gz generated in previous step
```

Then you should be able to use the library from R (e.g. the following command should give the installation status)
```
R --slave -e "packageDescription('scidbstrm')"
```
