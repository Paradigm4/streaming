rm(list=ls())
library(scidb)
library(base64enc)
scidbconnect()

# We store a generic R expression for use by the R processes
# invoked on each instance. The expression is replicated to
# each instance with data.
fn = expression(
  {
    print("Any R expression")       # these print statements are logged in
    print("can appear here...")     # scidb-stderr.log
    map(function(x) {data.frame(pi)+length(item)}) # be sure to match the 'types=...' parameter below
  #  map(function(x) {item = 10; data.frame(pi)+length(item)}) # Try the alternate version to visualize the scope of `item` above
  })
item = runif(50)
print(mean(item))
pkg = list(fn, item)
program <- as.scidb(base64encode(serialize(pkg, NULL)))

query = sprintf("stream(build(<v:double>[i=1:1,1,0],i), 
                'R --slave -e \"library(scidbstrm);pkg=unserialize(base64enc::base64decode(getChunk()[[1]]));item=pkg[[2]];print(mean(item));eval(pkg[[1]])\"', 
                'format=df', 'types=double', _sg(%s, 0))", program@name)
print(iquery(query, return=TRUE))
