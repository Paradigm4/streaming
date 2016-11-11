# Combine a bunch of Random Forest models into a single model

library(scidbstrm)
library(jsonlite)
library(randomForest)

result <- NULL

f <- function(x)
{
  model <- unserialize(base64_dec(x[,1]))
  if(is.null(result)) result <<- model 
  else result <<- combine(result, model)
  NULL
}

final <- function(x)
{
  if(!is.null(result))
  {
    return(data.frame(x=base64_enc(serialize(result, NULL)), stringsAsFactors=FALSE))
  }
  return(NULL)
}

map(f, final=final)
