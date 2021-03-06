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

library(poLCA)
library(jsonlite)
library(scidb)
con <- scidbconnect()

# The following program will be run by R processes invoked
# on each SciDB instance.
program <- as.scidb(con, base64_enc(serialize(expression(
{
  library(poLCA)
  library(parallel)
  library(jsonlite)
  library(scidbstrm)

  fn <- function(x)
  {
    # 1. Advance to the specified L'Ecuyer RNG stream
    RNGkind("L'Ecuyer-CMRG")
    set.seed(1)
    for(j in seq(from=1, to=x$seed[1]))
    {
      .Random.seed <<- nextRNGStream(.Random.seed)
    }

    # 2. Convert columns to factors, omit seed
    y <- data.frame(lapply(x[, -5], factor))

    # 3. Compute poLCA
    f <- cbind(PURPOSE, ACCURACY, UNDERSTA, COOPERAT) ~ 1
    p <- poLCA(f, y, nclass=4, maxiter=10000, tol=1e-7, verbose=FALSE)
    best <- p$llik

    # 4. Serialize result and return in 2-column data frame with
    # column 1: llik  (double precision value) best log likelihood
    # column 2: base64encoded serialized poLCA model
    model <- base64_enc(serialize(p, NULL))
    data.frame(llik=best, model=model, stringsAsFactors=FALSE)
  }
  map(fn)
}), NULL)))




# An example from the poLCA vignette:
data(gss82)
n <- nrow(gss82)
REPLICATIONS <-  nrow(iquery(con, "list('instances')", TRUE))

# Replicate and upload the data, adding integer seed values that vary by
# instance. This replication strategy is reproducible, even on different-sized
# SciDB clusters.
repl <- Reduce(rbind, Map(function(j) cbind(gss82, seed=j), 1:REPLICATIONS))

schema <- sprintf("<PURPOSE:string,ACCURACY:string,UNDERSTA:string,COOPERAT:string,seed:int32> [i=1:1202:0:%d]", n)
x <- store(con, con$repart(as.scidb(con, repl), R(schema)), name='gss82')


# Run the experiment using the SciDB streaming API in parallel.
query <- sprintf("stream(gss82,
                   'R --slave -e \"library(scidbstrm);eval(unserialize(jsonlite::base64_dec(getChunk()[[1]])))\"',
                   'format=df',
                   'types=double,string',
                   'names=llik,model',
                   _sg(%s,0))",
                 program@name)

result <- iquery(con, query, return=TRUE)

llik <- result$llik
# Convert the encoded models back into R objects
models <- lapply(result$model, function(x) unserialize(base64_dec(x)))

# The model with the best llik
print(models[which(llik == max(llik))[1]])
