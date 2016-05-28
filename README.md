# streaming
Prototype Hadoop streaming-like SciDB API

The operator sends SciDB array data into the stdin of the process and reads its
stdout (hence 'streaming').

## Usage
```
stream(ARRAY, PROGRAM, 'format=...', 'types=...', 'names=...')
```
where,

* ARRAY is a SciDB array expression
* PROGRAM is a full command line to the program to stream data through
* format is either `'format=df'` for R binary data frame format or `'format=tsv'` for tab-delimited text (the R binary format is provisional and will eventually be replaced by feather)
* types is a comma-separated list of expected returned column SciDB types.
* names is an optional set of comma-separated output column names and must be the same length as `types` (default names are a0, a1, ...)


## R package

See the package vignettes and source code in this sofware repository for more details.

- https://github.com/paradigm4/streaming/r_pkg/vignettes/basic_examples.Rmd
