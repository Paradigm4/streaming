# BEGIN_COPYRIGHT
#
# Copyright (C) 2017-2021 Paradigm4 Inc.
# All Rights Reserved.
#
# scidbbridge is a plugin for SciDB, an Open Source Array DBMS
# maintained by Paradigm4. See http://www.paradigm4.com/
#
# scidbbridge is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as
# published by the Free Software Foundation.
#
# scidbbridge is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY
# KIND, INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See the
# AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public
# License along with scidbbridge. If not, see
# <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT

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
