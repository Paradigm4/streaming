library(scidbstrm)
library(jsonlite)
library(randomForest)

#' little bootstrap function
#' Combine resample data {x, y} rep times and combine randomForest(x,y,ntree) models
#' @param x y Random Forest input data and training vector
#' @param N full data number of rows
#' @param ntree number of Random Forest trees
#' @param rep number of bootstrap repetitions
lb <- function(x, y, N, ntree, rep)
{ 
  n <- nrow(x)
  Reduce(combine, Map(function(dummy) {
    i <- sample(n, n, replace=TRUE)
    r <- randomForest(x[i, ], y[i], ntree=ntree, norm.votes=FALSE)
    # Sum replicated rows from the bootstrap and then reassign in order
    # (Note: can speed up this step with, for example, data.table!)
    d <- data.frame(r$votes, row.names=c())
    d <- aggregate(d, by=list(row=as.integer(rownames(r$votes))), FUN=sum)
    # Omit oob.times vector (cheap, we could order it and fill in like the vote matrix instead)
    r$oob.times <- 0
    i <- which((names(d) %in% colnames(r$votes)))
    # Assign summed votes back in to the model object 'votes' matrix
    V <- matrix(0, nrow=N, ncol=ncol(r$votes))
    V[d$row, ] = as.matrix(d[, i])
    r$votes <- V
    r
  }, 1:rep))
}

# Our SciDB streaming function explicitly applies consistent factor levels
# to the "class" variable, presented to us as character values from SciDB.
# We return the little bootstrapped Random Forest model as a serialized
# R object, encoded in a character string since the SciDB streaming API
# does not yet support binary blobs.

levels <- c("sitting", "sittingdown", "standing", "standingup", "walking")

f <- function(x)
{
  x$class <- factor(x$class, levels=levels)
  mdl <- lb(x[, -18], x[[18]], 82816, 10, 10)
  data.frame(x=base64_enc(serialize(mdl, NULL)), stringsAsFactors=FALSE)
}

map(f)
