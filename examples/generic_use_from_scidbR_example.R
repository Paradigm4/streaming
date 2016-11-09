library(scidb)
library(jsonlite)
scidbconnect()

# We store a generic R expression for use by the R processes
# invoked on each instance. The expression is replicated to
# each instance with data.
program <- as.scidb(base64_enc(serialize(expression(
{
  print("Any R expression")       # these print statements are logged in
  print("can appear here...")     # scidb-stderr.log
  map(function(x) data.frame(pi)) # be sure to match the 'types=...' parameter below
}), NULL)))

query = sprintf("stream(build(<v:double>[i=1:1,1,0],i), 'R --slave -e \"library(scidbstrm);eval(unserialize(jsonlite::base64_dec(getChunk()[[1]])))\"', 'format=df', 'types=double', _sg(%s, 0))", program@name)
print(iquery(query, return=TRUE))
