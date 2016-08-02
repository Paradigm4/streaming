# streaming
Prototype SciDB API similar to Hadoop Streaming. The operator sends SciDB array data into the stdin of the process and reads its stdout (hence 'streaming').

![image](https://cloud.githubusercontent.com/assets/2708498/16286948/b4b649d2-38ad-11e6-903f-489fdc532212.png)

## Usage
```
stream(ARRAY, PROGRAM [, 'format=...'][, 'types=...'][, 'names=...'][, ARRAY2])
```
where

* ARRAY is a SciDB array expression
* PROGRAM is a full command line to the child program to stream data through
* format is either `'format=tsv'` for the TSV interface or `'format=df'` for the R binary data.frame interface (see below); tsv is default
* types is a comma-separated list of expected returned column SciDB types - used only with `'format=df'`
* names is an optional set of comma-separated output column names and must be the same length as `types` - used only with `'format=df'`; default column names are a0,a1,...
* ARRAY2 is an optional second array; if used, data from this array will be streamed to the child first

## TSV Interface for Flexibility

For each local chunk, each SciDB instance will convert all the attributes into a block of TSV text, preceded by an integer with the total number of lines. For example:
```
3
1   \N   1.1
2    B    \N
3   CD   2.3
```
Only attributes are transferred, so `apply()` the dimensions if you need them. The child process is expected to fully consume the entire chunk and then output a response in the same format. SciDB then consumes the response and sends the next chunk. At the end of the exchange, SciDB sends to the child process a zero-length message like so:
```
0
```
Which means "end of interaction". After that, the child is required to return another response, and then may legally self-terminate or wait for SciDB to terminate it. Note that the child may return an empty `0` message in response to any of the messages from SciDB. To recap, `0` from SciDB to child means "no more data" whereas `0` from child to SciDB means "no data right now." 

The child responses are returned in an array of `<response:string> [instance_id, chunk_no]` with the "number of lines" header and the final newline character removed. Depending on the contents, one way to parse such an array would be using the deprecated `parse()` operator provided in https://github.com/paradigm4/accelerated_io_tools. We might re-consider its deprecated status given this newfound utility.

```
# Note that you will need to compile the program `examples/client.cpp` in order 
# for the next command to work. A `Makefile` is provided
$ iquery -aq "parse(stream(apply(build(<a:double>[i=1:10,10,0], random()%5), b, random()%10), '$MYDIR/examples/stream_test_client'), 'num_attributes=3')"
{source_instance_id,chunk_no,line_no} a0,a1,a2,error
{0,0,0} 'Hello','4','5',null
{0,0,1} 'Hello','1','3',null
{0,0,2} 'Hello','2','6',null
{0,0,3} 'Hello','4','0',null
{0,0,4} 'Hello','2','8',null
{0,0,5} 'Hello','3','7',null
{0,0,6} 'Hello','2','2',null
{0,0,7} 'Hello','2','8',null
{0,0,8} 'Hello','4','8',null
{0,0,9} 'Hello','2','6',null
{0,0,10} 'OK','thanks!',null,'short'
```

All SciDB missing codes are converted to `\N` when transferring to the child.

## DF Interface for Fast Transfer to R

Each chunk is converted to the binary representation of the R data.frame-like list, one column per attribute. Only double, string, or int32 attributes are allowed. For example:
```
list(a0=as.integer(c(1,2,3)), a1=c(NA, 'B', 'CD'), a2=c(1.1, NA, 2.3))
```
Similar to the TSV interface, an empty message is an empty list:
```
list()
```
Just like in the TSV case, SciDB shall send one message per chunk to the child, each time waiting for a response. SciDB then sends an empty message to the child at the end of the interaction. The child must respond to each message from SciDB using either an empty or non-empty message. For convenience, the names of the SciDB attributes are passed as names of the list elements. However, the names of the R output columns going in the other direction are disregarded. Instead, the user may specify attribute names with `names=`. The user must also specify the types of columns returned by the child process using `types=` - again using only string, double and int32. The returned data are split into attributes and returned as:
```<a0:type0, a1:type1,...>[instance_id, chunk_no, value_no]```
where `a0,a1,..` are default attribute names that may be overridden with `names=` and the types are as supplied. 

When sending data to child, all SciDB missing codes are converted to the R `NA`. In the opposite direction, R `NA` values are converted to SciDB `null` (missing code 0).

After Feather is a little more stable, we will probably switch to that.

A quick example:
```
$ iquery -aq "stream(apply(build(<val:double> [i=1:5,5,0], i), s, 'Hello'), 'Rscript $MYDIR/examples/R_identity.R', 'format=df', 'types=double,string', 'names=a,b')" 
{instance_id,chunk_no,value_no} a,b
{0,0,0} 1,'Hello'
{0,0,1} 2,'Hello'
{0,0,2} 3,'Hello'
{0,0,3} 4,'Hello'
{0,0,4} 5,'Hello'
```
The next section discusses the companion R package and shows some really cool examples.

## R package

See the package vignettes and source code in this sofware repository for more details.

- https://github.com/Paradigm4/streaming/blob/master/r_pkg/vignettes/basic_examples.Rmd
- https://github.com/Paradigm4/streaming/blob/master/r_pkg/vignettes/advanced_example.Rmd

For installing the R package, see: https://github.com/Paradigm4/streaming/blob/master/r_pkg/vignettes/advanced_example.Rmd#r

## Stability and Security

SciDB shall terminate all the child processes and cancel the query if any of the child processes deviate from the exchange protocol or exit early. SciDB shall aslo kill all the child processes if the query is cancelled for any reason.

Beyond that, the user assumes risks inherent in running arbitrary code next to a database: one should make sure the memory is adequate, child processes shouldn't fork other processes, ensure the security model is not compromised and so on.

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
