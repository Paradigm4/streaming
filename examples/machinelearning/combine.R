# Combine a bunch of Random Forest models into a single model

library(scidbstrm)
library(base64enc)
library(randomForest)

result <- NULL

f <- function(x)
{
  model <- unserialize(base64decode(x[,1]))
  if(is.null(result)) result <<- model 
  else result <<- combine(result, model)
  NULL
}

final <- function(x)
{
  if(!is.null(result))
  {
    return(data.frame(x=base64encode(serialize(result, NULL)), stringsAsFactors=FALSE))
  }
  return(NULL)
}

map(f, final=final)
