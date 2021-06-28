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
