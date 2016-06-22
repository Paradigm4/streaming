# streaming
Prototype Hadoop streaming-like SciDB API The operator sends SciDB array data into the stdin of the process and reads its
stdout (hence 'streaming').

![image](https://cloud.githubusercontent.com/assets/2708498/16281408/47db750a-3892-11e6-8d93-20420f717a53.png)

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

- https://github.com/Paradigm4/streaming/blob/master/r_pkg/vignettes/basic_examples.Rmd
- https://github.com/Paradigm4/streaming/blob/master/r_pkg/vignettes/advanced_example.Rmd

## Installation

The easiest way is to first set up dev_tools (https://github.com/paradigm4/dev_tools).
Then it goes something like this:
```
$ iquery -aq "load_library('dev_tools')"
Query was executed successfully

$ iquery -aq "install_github('paradigm4/streaming')"
{i} success
{0} true

$ iquery -aq "load_library('stream')"
Query was executed successfully

$ iquery -aq "stream(filter(build(<val:double>[i=0:0,1,0],0),false), 'printf \"1\nWhat is up?\n\"')"
{instance_id,chunk_no} response
{0,0} 'What is up?'
{1,0} 'What is up?'
{2,0} 'What is up?'
{3,0} 'What is up?'
```
