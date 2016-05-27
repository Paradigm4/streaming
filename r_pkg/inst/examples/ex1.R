# Compute the average of values per instance. Run these examples from a command line.
#
# Obtain the location of this example script:
# example=`R --slave -e "cat(system.file('examples/ex1.R', package='scidbstrm'))"`
#
# Example with one double-valued input attribute and one double-valued output attribute:
# iquery -aq "stream(build(<v:double> [i=1:16,1,0], i), 'Rscript $example','format=df', 'types=double')"
#
# With two attributes of differing input types and two double-valued output attributes:
# iquery -aq "stream(apply(build(<v:double> [i=1:16,1,0], i), w, int32(2 * v)), 'Rscript $example','format=df', 'types=double,double')"

library(scidbstrm)

rowcount <- 0
state   <- NULL

f <- function(x)
{
  # Update global rowcount value and state data frame
  rowcount <<- rowcount + nrow(x)
  state <<- data.frame(lapply(rbind(state, x), sum))
  NULL  # no output to SciDB
}

final <- function(x) data.frame(lapply(state, function(y) y / rowcount))

map(f, final=final)
