# Compute the rolling average of values per instance across a three-value window.
#
# Run this example from a command line as follows:
# example=`R --slave -e "cat(system.file('examples/ex2.R', package='scidbstrm'))"`
# iquery -aq "stream(apply(build(<v:double> [i=1:32,1,0], i), w, 2*v), 'Rscript $example','format=df', 'types=double,double')"

library(scidbstrm)

state   <- NULL

# Define a rolling average function for data frame columns.
# There are of course many other ways to do this! We stick with base R stats.
# Note the precaution of making sure that the returned types match the query specification!
roll <- function(x, bandwidth)
{
  kernel <- rep(1 / bandwidth, bandwidth)
  data.frame(lapply(x, function(y) as.double(na.omit(filter(y, kernel)))))
}

f <- function(x)
{
  state <<- rbind(state, x)
  if(nrow(state) < 3) return(NULL)  # no output to SciDB (not enough data yet)
  ans <- roll(state, 3)             # compute rolling average over our data frame columns
  state <<- tail(state, 2)          # only need to keep last two values
  ans
}

map(f)
