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

# Internal utility function
# @param out a data frame
# @return a list suitable for writing to SciDB
asTypedList <- function(out, convertFactor)
{
  if(is.null(out)) return(list())
  stopifnot(is.data.frame(out))
  out <- as.list(out)
  # limit types to double, int, logical
  types <- vapply(out, class, "")
  i <- types %in% "logical"
  if(any(i)) out[i] <- lapply(out[i], as.integer)
  i <- types %in% "factor"
  if(any(i)) out[i] <- lapply(out[i], convertFactor)
  out
}


# re-direct usual R output to stderr to avoid accidental interference with
# SciDB communication. This is important, but results in R CMD check errors.
.onAttach = function(libname,pkgname)
{
  sink(stderr())
}

# Glogbal state, if needed, can go here
.scidbstream.env <- new.env()
