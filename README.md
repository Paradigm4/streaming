# stream

[![Build Status](https://travis-ci.org/Paradigm4/stream.svg)](https://travis-ci.org/Paradigm4/stream)

Prototype SciDB API similar to Hadoop Streaming. The operator sends SciDB array data into the stdin of the process and reads its stdout (hence 'streaming').

![image](https://cloud.githubusercontent.com/assets/2708498/16286948/b4b649d2-38ad-11e6-903f-489fdc532212.png)

## Usage
```
stream(ARRAY [, ARRAY2], PROGRAM [, format:'...'][, types:('...')][, names:('...')])
```
where

* ARRAY is a SciDB array expression
* PROGRAM is a full command line to the child program to stream data through
* format is either `format:'tsv'` for the tab-separated values (TSV)
  interface, `format:'feather'` for Apache Arrow, Feather format, or
  `format:'df'` for the R binary data.frame interface (see below);
  `tsv` is the default
* types is a comma-separated list of expected returned column SciDB
  types - used only with `format:'df'`
* names is an optional set of comma-separated output column names and
  must be the same length as `types` - used only with `format:'df'`;
  default column names are a0,a1,...
* ARRAY2 is an optional second array; if used, data from this array
  will be streamed to the child first

## Communication Protocol

The SciDB `stream` operator communicates with the external child
program using the standard output, `stdout` pipe. The communication
protocol is as follows:

1. SciDB sends to the child one chunk of data
1. Child reads the entire data chunk from SciDB
1. Child sends a chunk of response data to SciDB. A `0`-size hunk is
   expected if the child has no output data
1. If more data is available on the SciDB side, proceed to step 1
1. Child sends a final chunk of response data to SciDB. A `0`-size
   chunk is expected if the child has no final data

## Data Transfer Format

Three data transfer formats are available, each with their own
strengths.

### Tab Separated Values (TSV) for Flexibility

For each local chunk, each SciDB instance will convert all the
attributes into a block of TSV text, preceded by an integer with the
total number of lines. For example:

```
3
1   \N   1.1
2    B    \N
3   CD   2.3
```

Only attributes are transferred, so `apply()` the dimensions if you
need them. The child process is expected to fully consume the entire
chunk and then output a response in the same format. SciDB then
consumes the response and sends the next chunk. At the end of the
exchange, SciDB sends to the child process a zero-length message like
so:

```
0
```

Which means "end of interaction". After that, the child is required to
return another response, and then may legally self-terminate or wait
for SciDB to terminate it. Note that the child may return an empty `0`
message in response to any of the messages from SciDB. To recap, `0`
from SciDB to child means "no more data" whereas `0` from child to
SciDB means "no data right now."

The child responses are returned in an array of `<response:string> [instance_id, chunk_no]` with the "number of lines" header and the final newline character removed. Depending on the contents, one way to parse such an array would be using the deprecated `parse()` operator provided in https://github.com/paradigm4/accelerated_io_tools. We might re-consider its deprecated status given this newfound utility.

```
# Note that you will need to compile the program `examples/client.cpp` in order
# for the next command to work. A `Makefile` is provided
$ iquery -aq "parse(stream(apply(build(<a:double>[i=1:10,10,0], random()%5), b, random()%10), '$MYDIR/examples/stream_test_client'), num_attributes:3)"
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

All SciDB missing codes are converted to `\N` when transferring to the
child.

### Apache Arrow/Feather for Fast Transfer

Each chunk is converted Apache Arrow and written to the output in
Feather format. The Feather data is preceded by its size in
bytes. The supported types are int64, double, string and binary.

Just like in the TSV case, SciDB shall send one message per chunk to
the child, each time waiting for a response. SciDB then sends an empty
message to the child at the end of the interaction. The child must
respond to each message from SciDB using either an empty or non-empty
message. The data from the child is expected in the same format, the
size in bytes followed by data in Feather format.

To use the Feather format, specify format:'feather` as an argument to
the `stream` operator. For data coming from the child process, the
type of each attribute has to be specified using the `types:...`
argument. The names of each attribute can be specified as well using
the `names:...` argument. See the Python
[example](py_pkg/examples/3-read-write.py) for more details.

For Python the [SciDB-stream](py_pkg/README.rst) library provides
functions for reading data from SciDB as Pandas DataFrames and for
sending Pandas DataFrames to SciDB.


### DataFrame Interface for Fast Transfer to R

Each chunk is converted to the binary representation of the R
data.frame-like list, one column per attribute. Only double, string,
or int32 attributes are allowed. For example:

```
list(a0=as.integer(c(1,2,3)), a1=c(NA, 'B', 'CD'), a2=c(1.1, NA, 2.3))
```

Similar to the TSV interface, an empty message is an empty list:

```
list()
```

Just like in the TSV case, SciDB shall send one message per chunk to
the child, each time waiting for a response. SciDB then sends an empty
message to the child at the end of the interaction. The child must
respond to each message from SciDB using either an empty or non-empty
message. For convenience, the names of the SciDB attributes are passed
as names of the list elements. However, the names of the R output
columns going in the other direction are disregarded. Instead, the
user may specify attribute names with `names:`. The user must also
specify the types of columns returned by the child process using
`types:` - again using only string, double and int32. The returned
data are split into attributes and returned as: ```<a0:type0,
a1:type1,...>[instance_id, chunk_no, value_no]``` where `a0,a1,..` are
default attribute names that may be overridden with `names:` and the
types are as supplied.

When sending data to child, all SciDB missing codes are converted to
the R `NA`. In the opposite direction, R `NA` values are converted to
SciDB `null` (missing code 0).

A quick example:

```
$ iquery -aq "stream(apply(build(<val:double> [i=1:5,5,0], i), s, 'Hello'), 'Rscript $MYDIR/examples/R_identity.R', format:'df', 'types:('double','string'), names:('a','b'))"
{instance_id,chunk_no,value_no} a,b
{0,0,0} 1,'Hello'
{0,0,1} 2,'Hello'
{0,0,2} 3,'Hello'
{0,0,3} 4,'Hello'
{0,0,4} 5,'Hello'
```

The next section discusses the companion R package and shows some
really cool examples.

## R package

See the package vignettes and source code in this software repository for more details.

- [Basic example: aggregates](https://github.com/Paradigm4/stream/blob/master/r_pkg/vignettes/basic_examples.Rmd)
- [Intermediate example: range joins](https://github.com/Paradigm4/stream/blob/master/r_pkg/vignettes/ranges.Rmd)
- [Advanced example: machine learning](https://github.com/Paradigm4/stream/blob/master/r_pkg/vignettes/advanced_example.Rmd)

For installing the R package, see: https://github.com/Paradigm4/stream/blob/master/r_pkg/vignettes/advanced_example.Rmd#r

## Python package

A Python library is available for the `stream` plugin from Python. The
library makes available a number of utility functions for interacting
with the `stream` operator. The data format used to interact with the
library is Pandas
[DataFrame](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html). Internally,
the data is transferred using the
[Apache Arrow](https://arrow.apache.org/) library. Please the the
package [README](py_pkg/README.rst) file for instructions on installing
the package, API documentation, and examples.

## Stability and Security

SciDB shall terminate all the child processes and cancel the query if any of the child processes deviate from the exchange protocol or exit early. SciDB shall also kill all the child processes if the query is cancelled for any reason.

### SciDB EE

When using the SciDB Enterprise Edition in `password` mode, the user must be at least in the `operator` role in order to run `stream()` with an arbitrary command. An optional list of approved commands can be created in the file `/opt/scidb/VV.VV/stream_allowed`, one command per line. The commands in that file are allowed for any user. This is in addition to all array read and write permissions that apply just like they do in all other operators. For example:

```bash
$ cat /tmp/foo.sh
echo 1
echo "Yo!"

#Root can run anything:
$ iquery --auth-file=/home/scidb/.scidb_root_auth -aq "stream(filter(build(<val:double>[i=0:0,1,0],0),false), '/tmp/foo.sh')"
{instance_id,chunk_no} response
{0,0} 'Yo!'
{1,0} 'Yo!'
{2,0} 'Yo!'
{3,0} 'Yo!'
{4,0} 'Yo!'
{5,0} 'Yo!'
{6,0} 'Yo!'
{7,0} 'Yo!'

#This user isn't in the 'operators' role:
$ iquery --auth-file=/home/scidb/.scidb_auth -aq "stream(filter(build(<val:double>[i=0:0,1,0],0),false), '/tmp/foo.sh')"
UserException in file: src/namespaces/CheckAccess.cpp function: operator() line: 73
Error id: libnamespaces::SCIDB_SE_QPROC::NAMESPACE_E_INSUFFICIENT_PERMISSIONS
Error description: Query processor error. Insufficient permissions, need {[(db:)A],} but only have {[(ns:XYZ)lr],[(ns:public)RAclrud],}.

#Add this command to the list of allowed commands:
$ echo "/tmp/foo.sh" >> /opt/scidb/16.9/etc/stream_allowed

#Now anyone can run this command:
$ iquery --auth-file=/home/scidb/.scidb_auth -aq "stream(filter(build(<val:double>[i=0:0,1,0],0),false), '/tmp/foo.sh')"
{instance_id,chunk_no} response
{0,0} 'Yo!'
{1,0} 'Yo!'
{2,0} 'Yo!'
{3,0} 'Yo!'
{4,0} 'Yo!'
{5,0} 'Yo!'
{6,0} 'Yo!'
{7,0} 'Yo!'
```

Beyond this, the user assumes risks inherent in running arbitrary code next to a database: one should make sure the memory is adequate, child processes shouldn't fork other processes, ensure the security model is not compromised and so on.

## Installation

### Install Apache Arrow

Due to a version conflict with the Protocol Buffers library included
with the official Apache Arrow packages, we use Apache Arrow packages
custom built for SciDB.

#### CentOS 6
```bash
> sudo yum install \
    https://dl.fedoraproject.org/pub/epel/epel-release-latest-6.noarch.rpm

> sudo wget --output-document /etc/yum.repos.d/bintray-rvernica-rpm.repo \
    https://bintray.com/rvernica/rpm/rpm
> sudo yum install arrow-devel
```

#### CentOS 7
```bash
> sudo yum install \
     https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

> sudo wget --output-document /etc/yum.repos.d/bintray-rvernica-rpm.repo \
    https://bintray.com/rvernica/rpm/rpm
> sudo yum install arrow-devel
```

#### Ubuntu
```bash
> cat <<APT_LINE | tee /etc/apt/sources.list.d/bintray-rvernica.list
deb https://dl.bintray.com/rvernica/deb trusty universe
APT_LINE

> apt-key adv --keyserver hkp://keyserver.ubuntu.com --recv 46BD98A354BA5235
> apt-get update
> apt-get install libarrow-dev libarrow0
```

### Install plug-in

The easiest way is to first set up dev_tools (https://github.com/paradigm4/dev_tools).
Then it goes something like this:
```
$ iquery -aq "load_library('dev_tools')"
Query was executed successfully

$ iquery -aq "install_github('paradigm4/stream')"
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
