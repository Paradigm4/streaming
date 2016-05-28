# streaming
Prototype Hadoop streaming-like SciDB API

The operator sends SciDB array data into the stdin of the process and reads its
stdout (hence 'streaming').

## Usage
```
stream(ARRAY, PROGRAM, 'format=...', 'types=...')
```
where,

* ARRAY is a SciDB array expression
* PROGRAM is a full command line to the program to stream data through
* format is either `'format=df'` for R binary data frame format or `'format=tsv'` for tab-delimited text (the R binary format is provisional and will eventually be replaced by feather)
* types is a comma-separated list of expected returned column SciDB types.

## Examples

